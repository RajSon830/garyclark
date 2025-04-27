<?php 

namespace Raj\Framework\Http\Middleware;

use Raj\Framework\Http\HttpException;
use Raj\Framework\Http\Middleware\MiddlewareInterface;
use Raj\Framework\Http\Middleware\RequestHandlerInterface;
use Raj\Framework\Http\Request;
use Raj\Framework\Http\Response;

use FastRoute\Dispatcher;
use FastRoute\RouteCollector;

use Raj\Framework\Http\HttpRequestMethodException;
use function FastRoute\simpleDispatcher;

class ExtractRouteInfo implements MiddlewareInterface{

    public function __construct(private array $routes){

    }

    public function process(Request $request,RequestHandlerInterface $requestHandler):Response{
        
        // create a dispatcher
        $dispatcher = simpleDispatcher(function (RouteCollector $routeCollector) {

            foreach ($this->routes as $route) {
                $routeCollector->addRoute(...$route);
            }
        });

        // Dispatch a URI, to obtain the route info
        $routeInfo = $dispatcher->dispatch(
            $request->getRequestMethod(),
            $request->getPathInfo()
        );

        switch ($routeInfo[0]) {

            case Dispatcher::FOUND:
                //return [$routeInfo[1],$routeInfo[2]];
                $request->setRouteHandler($routeInfo[1]);


                $request->setRouteHandlerArgs($routeInfo[2]);
                // set $request->routerHandler

                // Inject route middleware on handler
                if(is_array($routeInfo[1]) && isset($routeInfo[1][2])){

                
                }   

                break;

            case Dispatcher::METHOD_NOT_ALLOWED:
                $allowedMethods = implode(',',$routeInfo[1]);
                $e=new HttpRequestMethodException("This allowed methdos are $allowedMethods");
                $e->setStatusCode(405);
                throw $e;
            default:
                $e=new HttpException('Not Found');
                $e->setStatusCode(404);
                throw $e;
        }
       


        return $requestHandler->handle($request);
    }


}