<?php 

namespace Raj\Framework\Http\Middleware;

use Raj\Framework\Http\Middleware\MiddlewareInterface;
use Raj\Framework\Http\Middleware\RequestHandlerInterface;
use Raj\Framework\Http\Request;
use Raj\Framework\Http\Response;

class Success implements MiddlewareInterface{


    public function process(Request $reqeust,RequestHandlerInterface $requestHandler):Response
    {
        return new Response('OMG it is worked!!',200);
    }


}