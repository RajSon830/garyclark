<?php

use App\Controller\AbstractCotroller;
use League\Container\Argument\Literal\ArrayArgument;
use League\Container\Argument\Literal\StringArgument;
use League\Container\Container;
use League\Container\ReflectionContainer;
use Raj\Framework\Console\Application;
use Raj\Framework\Dbal\ConnectionFactory;
use Raj\Framework\Http\Kernel\Kernel;
use Raj\Framework\Routing\Router;
use Raj\Framework\Routing\RouterInterface;
use Raj\Framework\Session\SessionInterface;
use Raj\Framework\Template\TwigFactory;
use Symfony\Component\Dotenv\Dotenv;
use Twig\Environment;
use Twig\Loader\FilesystemLoader;
use Raj\Framework\Session\Session;

$routes = include BASE_PATH.'/routes/web.php';

/**
 * Database Url 
 */
$mysqlConfiguration = [
    'dbname'   => 'blogs',
    'user'     => 'root',
    'password' => '',
    'host'     => '127.0.0.1', // or 'localhost'
    'driver'   => 'pdo_mysql',
    'charset'  => 'utf8mb4',
];


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


$container->add('base-commands-namespace',new StringArgument('Raj\\Framework\\Console\\Command\\'));

// if find the perticular item which you are looking for, then it will use it.
$container->delegate(new ReflectionContainer(true));
# Parameters for application configuration 


// router will be initialize with RouterInterface 
$container->add(RouterInterface::class,Router::class);

// as it is initialized calle following method;
$container->extend(RouterInterface::class)->addMethodCall('setRoutes',[new ArrayArgument($routes)]);

$container->add(Kernel::class)->addArgument(RouterInterface::class)->addArgument($container);

$container->add(\Raj\Framework\Console\Kernel::class)->addArguments([$container,Application::class]);

/*
$container->addShared('filesystem-loader',FilesystemLoader::class)->addArgument(new StringArgument($templatesPath));

$container->addShared('twig',Environment::class)->addArgument('filesystem-loader');*/

$container->addShared(SessionInterface::class,Session::class);

$container->add('template-renderer-factory',TwigFactory::class)->addArguments([SessionInterface::class,new StringArgument($templatesPath)]);

$container->addShared('twig',function() use ($container){
    return $container->get('template-renderer-factory')->create();
});

$container->add(AbstractCotroller::class);

$container->inflector(AbstractCotroller::class)->invokeMethod('setContainer',[$container]);

$container->add(ConnectionFactory::class)->addArgument(new ArrayArgument($mysqlConfiguration));

$container->addShared(\Doctrine\DBAL\Connection::class,function() use ($container): \Doctrine\DBAL\Connection{
    return $container->get(ConnectionFactory::class)->create();
});

return $container;