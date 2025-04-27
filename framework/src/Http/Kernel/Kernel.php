<?php 

namespace Raj\Framework\Http\Kernel;

use Doctrine\DBAL\Connection;
use Raj\Framework\Http\Request;
use Raj\Framework\Http\Response;
use Exception;
use Psr\Container\ContainerInterface;
use Raj\Framework\Http\HttpException;
use Raj\Framework\Http\HttpRequestMethodException;
use Raj\Framework\Http\Middleware\RequestHandlerInterface;
use Raj\Framework\Routing\Router;
use Raj\Framework\Routing\RouterInterface;

class Kernel{
    
    private string $appEnv;

   
    public function __construct(
        private RouterInterface $router,
        private ContainerInterface $container,
        private RequestHandlerInterface $requestHandlerInterface
        ){

            $this->appEnv = $this->container->get('APP_ENV');

    }


    public function handle(Request $request){

        try{

            $response = $this->requestHandlerInterface->handle($request);
            #throw new Exception("Hekki");

            
        }catch(Exception $exception){

            $response = $this->createExceptionResponse($exception);
        }

        return $response;
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



    public function terminate(Request $request,Response $response){

        $request->getSession()?->clearFlash();
    }
}
