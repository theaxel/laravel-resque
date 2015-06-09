<?php
namespace Awellis13\Resque\Connectors;

use Config;
use Resque;
use Resque_Redis;
use Awellis13\Resque\ResqueQueue;
use Illuminate\Queue\Connectors\ConnectorInterface;

/**
 * Class ResqueConnector
 *
 * @package Resque\Connectors
 */
class ResqueConnector implements ConnectorInterface
{
    /**
     * {@inheritDoc}
     */
    public function connect(array $config)
    {
        $config = array_merge(Config::get('database.redis.default', []), $config);

        Resque::setBackend(
            array_get($config, 'host', Resque_Redis::DEFAULT_HOST) .':'. array_get($config, 'port', Resque_Redis::DEFAULT_PORT),
            array_get($config, 'database', Resque_Redis::DEFAULT_DATABASE)
        );

        return new ResqueQueue();
    }
}
