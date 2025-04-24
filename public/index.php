<?php 
declare(strict_types=1);
use App\Http\Kernel\Kernel;


//request received

//perform some logic

//send response (string of content)

define("BASE_ROOT",dirname(__DIR__));

// import autoload.php to file to setup autoloader.
require_once BASE_ROOT.'/vendor/autoload.php';


use App\Http\Request;
use App\Http\Response;

// Creating Request Factory Method
$request = Request::createFromGlobals();

#dd($request);

// Creating Response 

$content = "Hello World";

$kernel = new Kernel();

$response = $kernel->handle($request);

$response->send();