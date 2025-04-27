<?php 

namespace Raj\Framework\Http\Middleware;

use Raj\Framework\Http\Request;
use Raj\Framework\Http\Response;
use Raj\Framework\Http\Middleware\RequestHandlerInterface;

interface MiddlewareInterface
{
    public function process(Request $request,RequestHandlerInterface $requestHandler):Response;
}