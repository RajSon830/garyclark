<?php 

namespace Raj\Framework\Authentication;

use App\Entity\User;
use App\Repository\AuthRepositoryInterface;
use Raj\Framework\Authentication\AuthUserInterface;
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
        if(password_verify($password,$user->getPassword())){
            // you log in user in
            $this->login($user);

            return true;

        }


        // if yes, log the user in 

        // return true 

        return false;
    }

    public function login(AuthUserInterface $user){

        // Start a session
        $this->session->start();
       // Log the user in 
        $this->session->set('auth_id',$user->getAuthId());
        // Set the user 
        $this->user = $user;

    }

    public function logout(){

    }

    public function getUser(){

        return $this->user;
        
    }


}