<?php

/*
 * This file is part of the GraphAware Neo4j Client package.
 *
 * (c) GraphAware Limited <http://graphaware.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace GraphAware\Neo4j\Client\Connection;

use PTS\Bolt\Configuration as BoltConfiguration;
use PTS\Bolt\Driver as BoltDriver;
use PTS\Bolt\Exception\MessageFailureException;
use PTS\Bolt\GraphDatabase as BoltGraphDB;
use GraphAware\Common\Connection\BaseConfiguration;
use GraphAware\Common\Cypher\Statement;
use GraphAware\Neo4j\Client\Exception\Neo4jException;
use GraphAware\Neo4j\Client\HttpDriver\GraphDatabase as HttpGraphDB;
use GraphAware\Neo4j\Client\StackInterface;

class Connection
{
    /**
     * @var string The Connection Alias
     */
    private $alias;

    /**
     * @var string
     */
    private $uri;

    /**
     * @var \GraphAware\Common\Driver\DriverInterface The configured driver
     */
    private $driver;

    /**
     * @var array
     */
    private $config;

    /**
     * @var \GraphAware\Common\Driver\SessionInterface
     */
    private $session;

    // BOLT Routing parameters
    private $boltRouting = false;
    private $boltRoutingConfig = null;
    private $boltRoutingReadServers;
    private $boltRoutingWriteServers;
    private $boltRoutingLastMode = null;
    const BOLT_ROUTING_WRITE = "WRITE";
    const BOLT_ROUTING_READ = "READ";

    /**
     * Connection constructor.
     *
     * @param string                 $alias
     * @param string                 $uri
     * @param BaseConfiguration|null $config
     */
    public function __construct($alias, $uri, $config = null)
    {
        $this->alias = (string) $alias;
        $this->uri = (string) $uri;
        $this->config = $config;

        $this->buildDriver();
    }

    /**
     * @return string
     */
    public function getAlias()
    {
        return $this->alias;
    }

    /**
     * @return \GraphAware\Common\Driver\DriverInterface
     */
    public function getDriver()
    {
        return $this->driver;
    }

    /**
     * @param null  $query
     * @param array $parameters
     * @param null  $tag
     *
     * @return \GraphAware\Common\Driver\PipelineInterface
     */
    public function createPipeline($query = null, $parameters = [], $tag = null)
    {
        $this->checkSession();
        $parameters = is_array($parameters) ? $parameters : [];

        return $this->session->createPipeline($query, $parameters, $tag);
    }

    /**
     * @param string      $statement
     * @param array|null  $parameters
     * @param null|string $tag
     *
     * @throws Neo4jException
     *
     * @return \GraphAware\Common\Result\Result
     */
    public function run($statement, $parameters = null, $tag = null)
    {
        // Check bolt-routing mode
        $this->checkUpdateServerBoltRouting($statement);
        $this->checkSession();
        if (empty($statement)) {
            throw new \InvalidArgumentException(sprintf('Expected a non-empty Cypher statement, got "%s"', $statement));
        }
        $parameters = (array) $parameters;

        try {
            $result = $this->session->run($statement, $parameters, $tag);
            // Reset bolt-routing mode by default: WRITE
            $this->checkUpdateServerBoltRouting($statement, self::BOLT_ROUTING_WRITE);
            return $result;
        } catch (MessageFailureException $e) {
            $exception = new Neo4jException($e->getMessage());
            $exception->setNeo4jStatusCode($e->getStatusCode());

            throw $exception;
        }
    }

    /**
     * @param array $queue
     *
     * @return \GraphAware\Common\Result\ResultCollection
     */
    public function runMixed(array $queue)
    {
        $this->checkSession();
        $pipeline = $this->createPipeline();

        foreach ($queue as $element) {
            if ($element instanceof StackInterface) {
                foreach ($element->statements() as $statement) {
                    $pipeline->push($statement->text(), $statement->parameters(), $statement->getTag());
                }
            } elseif ($element instanceof Statement) {
                $pipeline->push($element->text(), $element->parameters(), $element->getTag());
            }
        }

        return $pipeline->run();
    }

    /**
     * @return \GraphAware\Common\Transaction\TransactionInterface
     */
    public function getTransaction()
    {
        $this->checkSession();

        return $this->session->transaction();
    }

    /**
     * @return \GraphAware\Common\Driver\SessionInterface
     */
    public function getSession()
    {
        $this->checkSession();

        return $this->session;
    }

    private function buildDriver()
    {
        $params = parse_url($this->uri);

        if (preg_match('/bolt/', $this->uri)) {
            $port = isset($params['port']) ? (int) $params['port'] : BoltDriver::DEFAULT_TCP_PORT;
            $uri = sprintf('%s://%s:%d', $params['scheme'], $params['host'], $port);
            if (isset($params['user']) && isset($params['pass'])) {
                $config = BoltConfiguration::create()
                    ->withCredentials($params['user'], $params['pass'])
                    ->withTLSMode(BoltConfiguration::TLSMODE_REQUIRED);
            } else {
                $config = BoltConfiguration::create()->withCredentials('null', 'null');
            }
            $this->driver = BoltGraphDB::driver($uri, $config, 1);
            // Check and setting WRITE by default
            $this->checkInitBoltRouting($config, self::BOLT_ROUTING_WRITE);
        } elseif (preg_match('/http/', $this->uri)) {
            $uri = $this->uri;
            $this->driver = HttpGraphDB::driver($uri, $this->config);
        } else {
            throw new \RuntimeException(sprintf('Unable to build a driver from uri "%s"', $this->uri));
        }
    }

    /** 
     * Initialize Routing Solution for Bolt
     * @param BoltConfiguration $config
     * @param string $forceMode - "WRITE" or "READ"
     * @return boolean - Return true if bolt-routing is configured
     */
    private function checkInitBoltRouting($config, $forceMode) {
        if (preg_match('/bolt-routing/', $this->uri) || preg_match('/bolt\+routing/', $this->uri)) {
            $this->boltRouting = true;
            $this->boltRoutingConfig = $config;
            $this->checkSession();
            $result = $this->session->run("CALL dbms.routing.getRoutingTable({})");
            $resultServers = $result->getRecords()[0]->values()[1];
            foreach($resultServers as $server) {
                if ($server['role'] == self::BOLT_ROUTING_WRITE) {
                    foreach ($server['addresses'] as $url) {
                        $this->boltRoutingWriteServers[] = $url;
                    }
                } else if ($server['role'] == self::BOLT_ROUTING_READ) {
                    foreach ($server['addresses'] as $url) {
                        $this->boltRoutingReadServers[] = $url;
                    }
                }
            }
            if ($forceMode != null) {
                $this->checkUpdateServerBoltRouting(null, $forceMode);
            }
            return true;
        }
        return false;
    }

    /**
     * Check if server need be updated for write or read
     * @param string $statement
     * @param bool $forceModeWrite
     * @return bool - Return true if server is changed
     * 
     */
    private function checkUpdateServerBoltRouting($statement, $forceMode = null) {
        if ($this->boltRouting) {
            $mode = preg_match('/(CREATE|SET|MERGE|DELETE)/m', $statement) ? self::BOLT_ROUTING_WRITE : self::BOLT_ROUTING_READ;
            if ($this->boltRoutingLastMode != self::BOLT_ROUTING_WRITE && ($mode === self::BOLT_ROUTING_WRITE || $forceMode == self::BOLT_ROUTING_WRITE)) {
                $this->boltRoutingLastMode = self::BOLT_ROUTING_WRITE;
                // Choose writer server
                $randServer = mt_rand(0, count($this->boltRoutingWriteServers) - 1);
                $this->driver = BoltGraphDB::driver($this->boltRoutingWriteServers[$randServer], $this->boltRoutingConfig, 1);
                // Reset Session
                $this->session = null;
                return true;
            } else if ($this->boltRoutingLastMode != self::BOLT_ROUTING_READ && ($mode === self::BOLT_ROUTING_READ || $forceMode == self::BOLT_ROUTING_READ)) {
                $this->boltRoutingLastMode = self::BOLT_ROUTING_READ;
                // Choose read server
                $randServer = mt_rand(0, count($this->boltRoutingReadServers) - 1);
                $this->driver = BoltGraphDB::driver($this->boltRoutingReadServers[$randServer], $this->boltRoutingConfig, 1);
                // Reset Session
                $this->session = null;
                return true;
            }
        }

        return false;
    }

    private function checkSession()
    {
        if (null === $this->session) {
            $this->session = $this->driver->session();
        }
    }
}
