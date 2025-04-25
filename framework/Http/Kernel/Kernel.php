<?php 

namespace Raj\Framework\Http\Kernel;

use Raj\Framework\Http\Request;
use Raj\Framework\Http\Response;
use Exception;
use Raj\Framework\Http\HttpException;
use Raj\Framework\Http\HttpRequestMethodException;
use Raj\Framework\Http\Routing\Router;

class Kernel{
    

    public function __construct(private Router $router){

    }


    public function handle(Request $request){


        try{

            [$routeHandler,$vars] = $this->router->dispatch($request);

            $resonse = call_user_func_array($routeHandler,$vars);

        }
        catch(HttpException $exception){
            $resonse = new Response($exception->getMessage(),$exception->getStatusCode());
        }
        catch(Exception $exception){
            $resonse = new Response($exception->getMessage(),500);
        }

        return $resonse;

       
    }


}
