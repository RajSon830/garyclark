<?php

namespace Raj\Framework\Http\Middleware;

use Psr\Container\ContainerInterface;
use Raj\Framework\Http\Middleware\RequestHandlerInterface;
use Raj\Framework\Http\Response;
use Raj\Framework\Http\Request;
use Raj\Framework\Http\Middleware\Authenticate;
use Raj\Framework\Http\Middleware\Success;


class RequestHandler implements RequestHandlerInterface{

    private array $middleware = [StartSession::class,Authenticate::class,RouterDispatch::class];

    public function __construct(private ContainerInterface $container){

    }

    public function handle(Request $request): Response
    {
        // If there are no middleware classes to execute, return a default response
        // A response should have been returned before the list becomes empty
        if (empty($this->middleware)) {
            return new Response("It's totally borked, mate. Contact support", 500);
        }

        // Get the next middleware class to execute
        $middlewareClass = array_shift($this->middleware);


        // Create a new instance of the middleware call process on it
        //$response = (new $middlewareClass())->process($request, $this);
        $middleware = $this->container->get($middlewareClass);

        $response = $middleware->process($request,$this);

        return $response;
    }
}