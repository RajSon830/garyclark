<?php 


namespace App\Controller;

use App\Form\User\RegistrationForm;
use App\Repository\UserMapper;
use Raj\Framework\Http\RedirectResponse;

class RegisterController extends AbstractCotroller{


        public function __construct(private UserMapper $userMapper){

        }

        public function index(){

            return $this->render('register.html.twig');
        }

        public function register(): RedirectResponse{
         
            // ceate a form model which will:
                //validate fields
                //map the fields to User object properties
                //ultimately save the new User to the DB
                
                $form = new RegistrationForm($this->userMapper);

                $form->setFields($this->request->getPostParams('username'),$this->request->getPostParams('password'));


                // validate 
                // If validation errors

                if($form->hasValidationErrors()){
                    //dd($form->getValidationErrors());
                    foreach($form->getValidationErrors() as $error){
                        $this->request->getSession()->setFlash('error',$error);
                    }

                    return new RedirectResponse('/register');
                }

                // add to session, redirect to form     
                $user = $form->save();

                #dd($user);

                // register the user by calling $form->save();
                $this->request->getSession()->setFlash('success',sprintf('User %s created',$user->getUsername()));

                // Add a session success message

                // Log the user in

                // Redirect to somewhere useful
                return new RedirectResponse('/');


        }

}