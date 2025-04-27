<?php 

namespace Raj\Framework\Authentication;

use  Raj\Framework\Authentication\Auth;

interface SessionAuthInterface{

    public function authenticate(string $username,string $password):bool;

    public function login(AuthUserInterface $user);

    public function logout();

    public function getUser();


}