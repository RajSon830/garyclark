<?php

use App\Controller\AbstractCotroller;
use League\Container\Argument\Literal\ArrayArgument;
use League\Container\Argument\Literal\StringArgument;
use League\Container\Container;
use League\Container\ReflectionContainer;
use Raj\Framework\Dbal\ConnectionFactory;
use Raj\Framework\Http\Kernel\Kernel;
use Raj\Framework\Routing\Router;
use Raj\Framework\Routing\RouterInterface;
use Symfony\Component\Dotenv\Dotenv;
use Twig\Environment;
use Twig\Loader\FilesystemLoader;

$routes = include BASE_PATH.'/routes/web.php';

/**
 * Database Url 
 */
$databaseUrl =  'sqlite:///'.BASE_PATH.'/var/db.sqlite';

/**
 * Twig location files loader 
 */
$templatesPath = BASE_PATH.'/templates';

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

$container->addShared('filesystem-loader',FilesystemLoader::class)->addArgument(new StringArgument($templatesPath));

$container->addShared('twig',Environment::class)->addArgument('filesystem-loader');

$container->add(AbstractCotroller::class);

$container->inflector(AbstractCotroller::class)->invokeMethod('setContainer',[$container]);

$container->add(ConnectionFactory::class)->addArgument(new StringArgument($databaseUrl));

$container->addShared(\Doctrine\DBAL\Connection::class,function() use ($container): \Doctrine\DBAL\Connection{
    return $container->get(ConnectionFactory::class)->create();
});

return $container;