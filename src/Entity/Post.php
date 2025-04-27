<?php 

namespace App\Entity;

use DateTimeImmutable;

class Post{

    public function __construct(
        private ?int $id,
        private string $title,
        private string $body,
        private \DateTimeImmutable $createedAt
    ){

    }

    public static function create(string $title,string $body,?int $id=null,?DateTimeImmutable $createdAt=null):Post{

        return new self($id,$title,$body,$createdAt ?? new DateTimeImmutable());
    }

    public function getTitle(){
        return $this->title;
    }

    public function getBody(){
        return $this->body;
    }

    public function getCreatedBy()  {
        return $this->createedAt;
    }

    public function setPostId(int $id){
        $this->id = $id;
    }

}