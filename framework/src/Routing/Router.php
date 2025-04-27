<?php

namespace Raj\Framework\Routing;

use App\Controller\AbstractCotroller;
use Psr\Container\ContainerInterface;
use Raj\Framework\Http\Request;

class Router implements RouterInterface
{
    public function dispatch(Request $request, ContainerInterface $container): array
    {
        $routeHandler = $request->getRouteHandler();
        $routeHandlerArgs = $request->getRouteHandlerArgs();

        if (is_array($routeHandler)) {
            [$controllerId, $method] = $routeHandler;
            $controller = $container->get($controllerId);
            if (is_subclass_of($controller, AbstractCotroller::class)) {
                $controller->setRequest($request);
            }
            $routeHandler = [$controller, $method];
        }

        return [$routeHandler, $routeHandlerArgs];
    }
}