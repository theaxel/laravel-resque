<?php namespace Awellis13\Resque\Connectors;

use Config;
use Resque;
use ResqueScheduler;
use Awellis13\Resque\ResqueQueue;
use Illuminate\Queue\Connectors\ConnectorInterface;

/**
 * Class ResqueConnector
 *
 * @package Resque\Connectors
 */
class ResqueConnector implements ConnectorInterface {

	/**
	 * Establish a queue connection.
	 *
	 * @param array $config
	 * @return \Illuminate\Queue\QueueInterface
	 */
	public function connect(array $config)
	{
		if (!isset($config['host']))
		{
			$config = Config::get('database.redis.default');

			if (!isset($config['host']))
			{
				$config['host'] = '127.0.0.1';
			}
		}

		if (!isset($config['port']))
		{
			$config['port'] = 6379;
		}

		if (!isset($config['database']))
		{
			$config['database'] = 0;
		}

		Resque::setBackend($config['host'].':'.$config['port'], $config['database']);

		return new ResqueQueue;
	}

} // End ResqueConnector
