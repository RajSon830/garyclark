<?php 

namespace App\Controller;

use Raj\Framework\Authentication\SessionAuthentication;
use Raj\Framework\Http\RedirectResponse;
use Raj\Framework\Http\Response;


class LoginController extends AbstractCotroller{

    public function __construct(private SessionAuthentication $authComponent){

    }

    public function index():Response{
        return $this->render('login.html.twig');
    }

    public function login(){
        //dd($this->request);


        $userIsAuthenticated = $this->authComponent->authenticate($this->request->getPostParams('username'),
        $this->request->getPostParams('password'));

        // Attemp to authenticate the user using a security component (bool)
        if(!$userIsAuthenticated){
            $this->request->getSession()->setFlash('error','Bad creds');
            return new RedirectResponse('/login');
        }
        // create a session for the user
        $user = $this->authComponent->getUser();

        // if successfully, retrieve the user
        $this->request->getSession()->setFlash('success','You are now logged in ');

        // Redirect the user to intended location
        return new RedirectResponse('/dashboard');

    }



    public function logout(): Response{

        // log the user out
        $this->authComponent->logout();

        // set a logout session massage
        $this->request->getSession()->setFlash('success','Bye.. See you soon');

        // redirect a login page
        return new RedirectResponse('/login');
    }
}