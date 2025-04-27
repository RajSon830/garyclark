<?php 

namespace Raj\Framework\Console\Command;

class MigrateDatabase implements CommandInterface
{
    public string $name  = 'database:migrations:migrate';

    public function execute(array $params = []): int{

        echo "hello";

        return 0;
    }
}

