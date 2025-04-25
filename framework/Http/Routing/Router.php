<?php

namespace Raj\Framework\Http\Routing;

use Exception;
use FastRoute\Dispatcher;
use Raj\Framework\Http\Routing\RouterInterface;
use Raj\Framework\Http\Request;
use FastRoute\RouteCollector;
use Raj\Framework\Http\HttpRequestMethodException;

use function FastRoute\simpleDispatcher;

class Router implements RouterInterface{

    public function dispatch(Request $request){

        $routerInfo = $this->extractRouteInfo($request);

        [$handler,$vars] = $routerInfo;

        [$controller,$method] = $handler;

        return [[new $controller,$method],$vars];

    }

    private function extractRouteInfo(Request $request){

        $dispatcher = simpleDispatcher(function(RouteCollector $routeCollector){

            $routes = include BASE_PATH.'/routes/web.php';

            foreach($routes as $route){
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
                throw new HttpRequestMethodException("This allowed methdos are $allowedMethods");
            default:
                throw new Exception('Not Found');

        }

    }

}