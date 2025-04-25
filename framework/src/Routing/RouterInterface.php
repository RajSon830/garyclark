<?php 

namespace Raj\Framework\Routing;

use Psr\Container\ContainerInterface;
use Raj\Framework\Http\Request;

interface RouterInterface{

    public function dispatch(Request $request,ContainerInterface $container);

    public function setRoutes(array $routes);
}