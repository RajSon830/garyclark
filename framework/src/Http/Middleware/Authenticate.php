<?php 

namespace Raj\Framework\Http\Middleware;

use Raj\Framework\Http\Response;
use Raj\Framework\Http\Request;

class Authenticate implements MiddlewareInterface{

    private bool $authenticated = true;

    public function process(Request $request,RequestHandlerInterface $requestHandlerInterface):Response{

        if(!$this->authenticated){
            return new Response('Authentication failed',401);
        }

        return $requestHandlerInterface->handle($request);

    }
}