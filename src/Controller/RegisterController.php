<?php 


namespace App\Controller;

class RegisterController extends AbstractCotroller{

        public function index(){

            return $this->render('register.html.twig');
        }

}