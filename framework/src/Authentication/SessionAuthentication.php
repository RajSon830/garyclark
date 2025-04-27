<?php 

namespace Raj\Framework\Authentication;

use App\Entity\User;
use App\Repository\AuthRepositoryInterface;
use Raj\Framework\Authentication\AuthUserInterface;
use Raj\Framework\Session\Session;
use Raj\Framework\Session\SessionInterface;

class SessionAuthentication implements SessionAuthInterface{

    private AuthUserInterface $user;

    public function __construct(private AuthRepositoryInterface $authRepositoryInterface,
    private SessionInterface $session){

    }

    public function authenticate(string $username,string $password):bool{

        //query db for user using username
        $user = $this->authRepositoryInterface->findByUsername($username);

        if(!$user){
            return false;
        }

        // does the hashed user pw match the hash of the attempted passsword
        if(!password_verify($password,$user->getPassword())){
            // you log in user in
            return false;
        }


        // if yes, log the user in 
        $this->login($user);

        // return true 

        return true;
    }

    public function login(AuthUserInterface $user){

        // Start a session
        $this->session->start();
       // Log the user in 
        $this->session->set(Session::AUTH_KEY,$user->getAuthId());
        // Set the user 
        $this->user = $user;

    }

    public function logout(){

        $this->session->remove(Session::AUTH_KEY);

    }

    public function getUser(){

        return $this->user;
        
    }


}