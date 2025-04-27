<?php 

namespace Raj\Framework\Console;

use DirectoryIterator;
use Raj\Framework\Console\Application;
use Psr\Container\ContainerInterface;
use Raj\Framework\Console\Command\CommandInterface;
use ReflectionClass;

final class Kernel{

    public function __construct(private ContainerInterface $container,private Application $application){

    }

    public function handle():int{
        
        
        // Register commands with the container
        $this->registerCommands();

        // Run the console application, returning a status code
        $status = $this->application->run();

        // return the status code
        return $status;
    }

    private function registerCommands(){
       // == Register All built In Commands ===
       
       $commandFiles = new DirectoryIterator(__DIR__.'/Command');

       $namespace = $this->container->get('base-commands-namespace');
       #dd($commandFiles);
       // Get all files in the command dir

       foreach($commandFiles as $commandFile){

            if(!$commandFile->isFile()){
                continue;
            }
       }
    
       // Loop over all files in the commands folder
       $command = $namespace.pathinfo($commandFile,PATHINFO_FILENAME);

       // Get the command class name  using psr4 this will be same as filename

       // if it is a subclass of CommandInterface
       if(is_subclass_of($command,CommandInterface::class)){

        $commandName = (new ReflectionClass($command))->getProperty('name')->getDefaultValue();
  
        $this->container->add($commandName, $command);
       }

       // Add to the container using the name as the ID e.g $container->add 
       // ('database:migrations:migrate',MigrateDatabase::class)
    }

}