<?php 

namespace App\Http\Kernel;

use App\Http\Request;
use App\Http\Response;

class Kernel{
    
    public function handle(Request $request){

        $content = "<h1>Hello World</h1>";

        return new Response($content);
    }


}
