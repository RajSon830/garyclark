<?php

namespace Raj\Framework\Middleware;

use Raj\Framework\Http\Middleware\MiddlewareInterface;
use Raj\Framework\Http\Middleware\RequestHandlerInterface;
use Raj\Framework\Http\Request;
use Raj\Framework\Http\Response;

class Dummy implements MiddlewareInterface{

    public function process(Request $request,RequestHandlerInterface $requestHandler): Response{

        return $requestHandler->handle($request);
    }

}