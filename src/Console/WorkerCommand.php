<?php
namespace Awellis13\Resque\Console;

#use Illuminate\Console\Command;
use Illuminate\Queue\Console\WorkCommand as Command;
use Symfony\Component\Console\Input\InputDefinition;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

/**
 * Overwrites some behavior of the provided worker command from laravel in the way
 * that the worker gets initialzed when the command is executed, not constructed.
 * Was done to already inject the queues and connection to the worker.
 *
 * @package Resque\Console
 */
class WorkerCommand extends Command
{
    /**
     * The console command name.
     *
     * @var string
     */
    protected $name = 'queue:work';

    protected $description = 'Run a resque worker';

    /**
     * Create a new command instance.
     *
     *
     * @return void
     */
    public function __construct()
    {
        $this->setDefinition(new InputDefinition());

        $this->setName($this->name);
        $this->setDescription('Run a resque worker');
        $this->configure();

        $this->specifyParameters();
    }

    /**
     * Execute the console command.
     *
     * @return void
     */
    public function fire()
    {
        $this->worker = $this->laravel->make('queue.worker', $this->option('queue'), $this->argument('connection'));

        parent::fire();
    }
}
