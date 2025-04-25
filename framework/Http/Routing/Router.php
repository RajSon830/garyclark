<?php

namespace Raj\Framework\Http\Routing;

use Raj\Framework\Http\Routing\RouterInterface;
use Raj\Framework\Http\Request;
use FastRoute\RouteCollector;
use function FastRoute\simpleDispatcher;

class Router implements RouterInterface{

    public function dispatch(Request $request){

        $dispatcher = simpleDispatcher(function(RouteCollector $routeCollector){

            $routes = include BASE_PATH.'/routes/web.php';

            foreach($routes as $route){
                // unpacking $route array store in web.php
                $routeCollector->addRoute(...$route);
            }

        });

        $routeInfo = $dispatcher->dispatch($request->getRequestMethod(),$request->getPathInfo());

        [$status,[$controller,$methods],$vars] = $routeInfo;

        return [[new $controller,$methods],$vars];

    }

}