<?php

namespace Raj\Framework\Routing;

use App\Controller\AbstractCotroller;
use Exception;
use FastRoute\Dispatcher;
use Raj\Framework\Routing\RouterInterface;
use Raj\Framework\Http\Request;
use FastRoute\RouteCollector;
use Psr\Container\ContainerInterface;
use Raj\Framework\Http\HttpException;
use Raj\Framework\Http\HttpRequestMethodException;

use function FastRoute\simpleDispatcher;

class Router implements RouterInterface{

    private array $routes;

    public function dispatch(Request $request,ContainerInterface $container){

        $routerInfo = $this->extractRouteInfo($request);

        [$handler,$vars] = $routerInfo;

        /**
         * Checking is it array or not because it might be possible it is just a function instead of Controller
         * 
         */
        if(is_array($handler)){
            [$controllerId,$method]=$handler;
            $controller = $container->get($controllerId);

            if(is_subclass_of($controller,AbstractCotroller::class)){
                
                $controller->setRequest($request);
            }


            $handler = [$controller,$method];
        }

        //$vars['request']=$request;

        return [$handler,$vars];

    }

    private function extractRouteInfo(Request $request){

        $dispatcher = simpleDispatcher(function(RouteCollector $routeCollector){

            //$routes = include BASE_PATH.'/routes/web.php';

            foreach($this->routes as $route){
                // unpacking $route array store in web.php
                $routeCollector->addRoute(...$route);
            }

        });

        $routeInfo = $dispatcher->dispatch($request->getRequestMethod(),$request->getPathInfo());

        switch($routeInfo[0]){

            case Dispatcher::FOUND:
                return [$routeInfo[1],$routeInfo[2]];
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

    }


    public function setRoutes(array $routes)
    {
        $this->routes = $routes;
    }

}