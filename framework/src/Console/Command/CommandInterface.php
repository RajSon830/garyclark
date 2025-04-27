<?php 

namespace Raj\Framework\Console\Command;

interface CommandInterface{
    public function execute(array $params = []):int; // php bin\console database:migrations:migrate 
}

