<?php 

namespace App\Repository;

use Doctrine\DBAL\Connection;
use App\Entity\User;

class UserMapper{

    public function __construct(private Connection $connection){

    }

    public function save(User $user){

        $stmt = $this->connection->prepare("INSERT INTO users (username,password_hash,created_at) VALUES (:username,:password_hash,:created_at)");

        $stmt->bindValue(':username',$user->getUsername());

        $stmt->bindValue(':password_hash',$user->getPassword());

        $stmt->bindValue(':created_at',$user->getCreatedAt()->format('Y-m-d H:i:s'));

        $stmt->executeStatement();

        $id = $this->connection->lastInsertId();

        $user->setId($id);

    }

}