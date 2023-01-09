<?php

namespace GraphAware\Neo4j\Client\Tests;

trait ConnectionUrlProvider
{
    protected function getConnections()
    {
        $host = 'localhost';
        $port = ':7474';
        $auth = '';
        if (getenv('NEO4J_HOST')) {
            $host = getenv('NEO4J_HOST');
        }
        if (getenv('NEO4J_PORT')) {
            $port = ':'.getenv('NEO4J_PORT');
        }
        if (getenv('NEO4J_USER')) {
            $auth = sprintf(
                '%s:%s@',
                getenv('NEO4J_USER'),
                getenv('NEO4J_PASSWORD')
            );
        }
        $uri = '://'.$auth.$host;
        
        return [
            'http' => 'http'.$uri.$port,
            'bolt' => 'bolt'.$uri
        ];
    }
}
