<?php 

namespace Raj\Framework\Http\Kernel;

use Raj\Framework\Http\Request;
use Raj\Framework\Http\Response;
use Exception;
use Raj\Framework\Http\Routing\Router;

class Kernel{
    

    public function __construct(private Router $router){

    }


    public function handle(Request $request){


        try{

            [$routeHandler,$vars] = $this->router->dispatch($request);

            $resonse = call_user_func_array($routeHandler,$vars);

        }catch(Exception $exception){

            $resonse = new Response($exception->getMessage(),400);
        }

        return $resonse;

       
    }


}
