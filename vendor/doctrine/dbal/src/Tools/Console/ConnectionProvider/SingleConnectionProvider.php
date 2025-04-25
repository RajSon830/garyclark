<?php

declare(strict_types=1);

namespace Doctrine\DBAL\Tools\Console\ConnectionProvider;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Tools\Console\ConnectionNotFound;
use Doctrine\DBAL\Tools\Console\ConnectionProvider;

use function sprintf;

final readonly class SingleConnectionProvider implements ConnectionProvider
{
    public function __construct(
        private Connection $connection,
        private string $defaultConnectionName = 'default',
    ) {
    }

    public function getDefaultConnection(): Connection
    {
        return $this->connection;
    }

    public function getConnection(string $name): Connection
    {
        if ($name !== $this->defaultConnectionName) {
            throw new ConnectionNotFound(sprintf('Connection with name "%s" does not exist.', $name));
        }

        return $this->connection;
    }
}
