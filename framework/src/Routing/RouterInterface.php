<?php

namespace Raj\Framework\Routing;

use Raj\Framework\Http\Request;
use Psr\Container\ContainerInterface;

interface RouterInterface
{
    public function dispatch(Request $request, ContainerInterface $container);
}