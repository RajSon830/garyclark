<?php 

namespace App\Http\Kernel;

use App\Http\Request;
use App\Http\Response;
use FastRoute\RouteCollector;

use function FastRoute\simpleDispatcher;

class Kernel{
    
    public function handle(Request $request){

        $dispatcher = simpleDispatcher(function(RouteCollector $routeCollector){

            $routeCollector->addRoute('GET','/',function(){
                $content = "<h1>Hello World</h1>";
                return new Response($content);

            });

        });

        $routeInfo = $dispatcher->dispatch($request->getRequestMethod(),$request->getRequestUri());

       [$status,$handler,$vars] = $routeInfo;

       return $handler($vars);


    }


}
