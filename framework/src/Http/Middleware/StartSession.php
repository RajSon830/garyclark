<?php 


namespace Raj\Framework\Http\Middleware;

use Raj\Framework\Http\Request;
use Raj\Framework\Http\Response;
use Raj\Framework\Session\SessionInterface;

class StartSession implements MiddlewareInterface{



    public function __construct(private SessionInterface $session, private string $apiPrefix = '/api/'){

    }

    public function process(Request $request, RequestHandlerInterface $requestHandler): Response
    {   
        if(!str_starts_with($request->getPathInfo(),$this->apiPrefix)){

            $this->session->start();

            $request->setSession($this->session);
        }

       

        return $requestHandler->handle($request);
    }

}