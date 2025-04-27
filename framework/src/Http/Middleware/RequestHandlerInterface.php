<?php 

namespace Raj\Framework\Http\Middleware;

use Raj\Framework\Http\Request;
use Raj\Framework\Http\Response;

interface RequestHandlerInterface{

    public function handle(Request $request):Response;

}