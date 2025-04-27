<?php 
declare(strict_types=1);

use Raj\Framework\Http\Kernel\Kernel;

//request received

//perform some logic

//send response (string of content)

define("BASE_PATH",dirname(__DIR__));

// import autoload.php to file to setup autoloader.
require_once BASE_PATH.'/vendor/autoload.php';

// League Container
$container = require BASE_PATH.'/config/services.php';

use Raj\Framework\Http\Request;

// Creating Request Factory Method
$request = Request::createFromGlobals();

#dd($request);

// Creating Response 

$content = "Hello World";

#$router = new Router();

#$kernel = new Kernel($router);

$kernel = $container->get(Kernel::class);

$response = $kernel->handle($request);

#dd($response);

$response->send();

$kernel->terminate($request,$response);