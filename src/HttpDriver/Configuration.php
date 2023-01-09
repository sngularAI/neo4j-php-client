<?php

/*
 * This file is part of the GraphAware Neo4j Client package.
 *
 * (c) GraphAware Limited <http://graphaware.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace GraphAware\Neo4j\Client\HttpDriver;

use Http\Client\HttpClient;
use Http\Discovery\HttpClientDiscovery;
use Http\Discovery\MessageFactoryDiscovery;
use Http\Message\RequestFactory;
use GraphAware\Common\Connection\BaseConfiguration;

/**
 * @author Tobias Nyholm <tobias.nyholm@gmail.com>
 */
class Configuration extends BaseConfiguration
{
    /**
     * @return Configuration
     */
    public static function create(HttpClient $httpClient = null, RequestFactory $requestFactory = null)
    {
        return new self([
            'http_client' => $httpClient ?: HttpClientDiscovery::find(),
            'request_factory' => $requestFactory ?: MessageFactoryDiscovery::find(),
        ]);
    }

    /**
     * @param HttpClient $httpClient
     *
     * @return Configuration
     */
    public function setHttpClient(HttpClient $httpClient)
    {
        return $this->setValue('http_client', $httpClient);
    }

    /**
     * @param RequestFactory $requestFactory
     *
     * @return Configuration
     */
    public function setRequestFactory(RequestFactory $requestFactory)
    {
        return $this->setValue('request_factory', $requestFactory);
    }
}
