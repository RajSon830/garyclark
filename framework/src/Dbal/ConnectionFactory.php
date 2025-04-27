<?php 


namespace Raj\Framework\Dbal;

use Doctrine\DBAL\DriverManager;

class ConnectionFactory{

    public function __construct(private array $databaseConfigurationArray){

    }

    public function create(){
        return DriverManager::getConnection($this->databaseConfigurationArray);
    }

}