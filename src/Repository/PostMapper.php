<?php 

namespace App\Repository;

use Doctrine\DBAL\Connection;
use App\Entity\Post;

class PostMapper{

    public function __construct(private Connection $connection){

    }

    public function save(Post $post):void{

        $stmt = $this->connection->prepare("INSERT INTO post (title,body,created_at) VALUES (:title,:body,:created_at)");

        $stmt->bindValue(':title',$post->getTitle());
        $stmt->bindValue(':body',$post->getBody());
        $stmt->bindValue('created_at',$post->getCreatedBy()->format('Y-m-d H:i:s'));
        $stmt->executeStatement();
        $id = $this->connection->lastInsertId();
        $post->setPostId($id);
    }
}