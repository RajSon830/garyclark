<?php 

namespace Raj\Framework\Http\Kernel;

use Doctrine\DBAL\Connection;
use Raj\Framework\Http\Request;
use Raj\Framework\Http\Response;
use Exception;
use Psr\Container\ContainerInterface;
use Raj\Framework\Http\HttpException;
use Raj\Framework\Http\HttpRequestMethodException;
use Raj\Framework\Routing\Router;
use Raj\Framework\Routing\RouterInterface;

class Kernel{
    
    private string $appEnv;

   
    public function __construct(
        private RouterInterface $router,
        private ContainerInterface $container){

            $this->appEnv = $this->container->get('APP_ENV');

    }


    public function handle(Request $request){

        try{

            #throw new Exception("Hekki");

            [$routeHandler,$vars] = $this->router->dispatch($request,$this->container);

            $resonse = call_user_func_array($routeHandler,$vars);

        }catch(Exception $exception){

            $resonse = $this->createExceptionResponse($exception);
        }

        return $resonse;
    }

    /**
     * @throws Exception
     */
    private function createExceptionResponse(Exception $exception):Response{

        if(in_array($this->appEnv,['dev','test'])){
            throw $exception; 
        }

        if($exception instanceof HttpException){
            return new Response($exception->getMessage(),$exception->getStatusCode());
        }
    
        return new Response('Server Error',Response::HTTP_INTERNET_SERVER_ERROR);

    }

}
