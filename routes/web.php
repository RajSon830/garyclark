<?php

use App\Controller\HomeController;
use App\Controller\PostController;
use App\Controller\RegisterController;
use Raj\Framework\Http\Response;


return [
    ['GET','/',[HomeController::class,'index']],
    ['GET','/post/{id:\d+}',[PostController::class,'show']],
    ['GET','/post',[PostController::class,'create']],
    ['POST','/post',[PostController::class,'store']],
    ['GET','/hello/{name:.+}',function(string $name){
        return new Response("Hello World ". $name);
    }],
    ['GET','/register',[RegisterController::class,'index']]
];