<?php 

namespace Raj\Framework\Http\Middleware;

use Psr\Container\ContainerInterface;
use Raj\Framework\Http\Response;
use Raj\Framework\Http\Request;
use Raj\Framework\Routing\RouterInterface;

class RouterDispatch implements MiddlewareInterface{

    public function __construct(private RouterInterface $router,private ContainerInterface $container){

    }

    public function process(Request $request, RequestHandlerInterface $requestHandler):Response{

        [$routeHandler,$vars] = $this->router->dispatch($request,$this->container);

        $resonse = call_user_func_array($routeHandler,$vars);

        #dd($resonse);

        return $resonse;
    }


}