<?php 

namespace App\Repository;

use Raj\Framework\Authentication\AuthUserInterface;

interface AuthRepositoryInterface{

    public function findByUsername(string $username):?AuthUserInterface;


}