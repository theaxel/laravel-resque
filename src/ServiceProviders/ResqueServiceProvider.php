<?php
namespace Awellis13\Resque\ServiceProviders;

use Config;
use Awellis13\Resque\Connectors\ResqueConnector;
use Awellis13\Resque\Console\ListenCommand;
use Awellis13\Resque\Console\WorkerCommand;
use Awellis13\Resque\ResqueWorker;
use Illuminate\Queue\QueueServiceProvider;

/**
 * Class ResqueServiceProvider
 *
 * @package Resque\ServiceProviders
 */
class ResqueServiceProvider extends QueueServiceProvider
{
    /**
     * {@inheritdoc}
     */
    public function registerConnectors($manager)
    {
        parent::registerConnectors($manager);
        $this->registerResqueConnector($manager);
    }

    /**
     * {@inheritdoc}
     */
    public function boot()
    {
        parent::boot();

        $this->registerCommand();
    }

    /**
     * Register the Resque queue connector.
     *
     * @param  \Illuminate\Queue\QueueManager $manager
     * @return void
     */
    protected function registerResqueConnector($manager)
    {
        $manager->addConnector('resque', function () {

            $redisConfig = Config::get('database.redis.default', []);
            $resqueConfig = Config::get('queue.connections.resque', []);
            Config::set('queue.connections.resque', array_merge($redisConfig, $resqueConfig, ['driver' => 'resque']));

            return new ResqueConnector();
        });
    }

    /**
     * Registers the artisan command.
     *
     * @return void
     */
    protected function registerCommand()
    {
        $this->app['command.resque.listen'] = $this->app->share(function ($app) {
            return new ListenCommand;
        });

        $this->commands('command.resque.listen');
    }

    /**
     * {@inheritdoc}
     */
    protected function registerWorker()
    {
        $this->registerWorkCommand();

        $this->registerRestartCommand();

        $this->app->singleton('queue.worker', function ($app, $queues, $connectionName = null) {
            return new ResqueWorker($app['queue'], $connectionName, $queues, $app['queue.failer'], $app['events']);
        });
    }

    /**
     * {@inheritdoc}
     */
    protected function registerWorkCommand()
    {
        $this->app->singleton('command.queue.work', function ($app) {
            return new WorkerCommand();
        });

        $this->commands('command.queue.work');
    }
}
