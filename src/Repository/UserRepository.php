<?php 

namespace App\Repository;

use App\Repository\AuthRepositoryInterface;
use Doctrine\DBAL\Connection;
use Raj\Framework\Authentication\AuthUserInterface;
use App\Entity\User;

class UserRepository implements AuthRepositoryInterface
{

    public function __construct(private Connection $connection) {
        
    }

    public function findByUsername(string $username): ?AuthUserInterface
    {
        // Create a query builder
        $queryBuilder = $this->connection->createQueryBuilder();

        $queryBuilder
            ->select('id', 'username', 'password_hash', 'created_at')
            ->from('users')
            ->where('username = :username')
            ->setParameter('username', $username);

        $result = $queryBuilder->executeQuery();

        $row = $result->fetchAssociative();

        if (!$row) {
            return null;
        }

        $user = new User(
            id: $row['id'],
            username: $row['username'],
            password: $row['password_hash'],
            createdAt: new \DateTimeImmutable($row['created_at'])
        );

        return $user;
    }
}