<?php 

use League\Container\Argument\Literal\ArrayArgument;
use League\Container\Argument\Literal\StringArgument;
use League\Container\Container;
use League\Container\ReflectionContainer;
use Raj\Framework\Http\Kernel\Kernel;
use Raj\Framework\Routing\Router;
use Raj\Framework\Routing\RouterInterface;
use Symfony\Component\Dotenv\Dotenv;

$routes = include BASE_PATH.'/routes/web.php';

/**
 * Loading Environemnt file 
 */
$dotenv = new Dotenv();
$dotenv->load(BASE_PATH.'/.env');
// finish loading environment file

$appEnv = $_SERVER['APP_ENV'];

$container = new Container();

$container->add('APP_ENV',new StringArgument($appEnv));

// if find the perticular item which you are looking for, then it will use it.
$container->delegate(new ReflectionContainer(true));
# Parameters for application configuration 


// router will be initialize with RouterInterface 
$container->add(RouterInterface::class,Router::class);

// as it is initialized calle following method;
$container->extend(RouterInterface::class)->addMethodCall('setRoutes',[new ArrayArgument($routes)]);

$container->add(Kernel::class)->addArgument(RouterInterface::class)->addArgument($container);

return $container;