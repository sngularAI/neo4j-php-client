<?php

/*
 * This file is part of the GraphAware Neo4j Client package.
 *
 * (c) GraphAware Limited <http://graphaware.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace GraphAware\Neo4j\Client\Tests\Integration;

use GraphAware\Neo4j\Client\ClientBuilder;
use GraphAware\Neo4j\Client\Tests\ConnectionUrlProvider;

class IntegrationTestCase extends \PHPUnit_Framework_TestCase
{
    use ConnectionUrlProvider;
    /**
     * @var \GraphAware\Neo4j\Client\Client
     */
    protected $client;

    public function setUp()
    {
        $connections = array_merge($this->getConnections(), $this->getAdditionalConnections());

        $this->client = ClientBuilder::create()
            ->addConnection('bolt', $connections['bolt'])
            ->addConnection('http', $connections['http'])
            ->build();
    }

    protected function getAdditionalConnections()
    {
        return [];
    }

    /**
     * Empties the graph database.
     *
     * @void
     */
    public function emptyDb()
    {
        $this->client->run('MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE r,n', null, null);
    }
}
