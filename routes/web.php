<?php

use App\Controller\HomeController;
use App\Controller\PostController;
use App\Controller\RegisterController;
use App\Controller\LoginController;
use App\Controller\DashboardController;
use Raj\Framework\Http\Middleware\Authenticate;
use Raj\Framework\Http\Middleware\Guest;
use Raj\Framework\Http\Response;
use Raj\Framework\Middleware\Dummy;

return [
    ['GET', '/', [HomeController::class, 'index']],
    ['GET', '/post/{id:\d+}', [PostController::class, 'show']],
    ['GET', '/post', [PostController::class, 'create']],
    ['POST', '/post', [PostController::class, 'store']],
    ['GET', '/hello/{name:.+}', function (string $name) {
        return new Response("Hello World " . $name);
    }],
    ['GET', '/register', [RegisterController::class, 'index'], [
        Guest::class
    ]],
    ['POST', '/register', [RegisterController::class, 'register']],
    ['GET', '/login', [LoginController::class, 'index']],
    ['POST', '/login', [LoginController::class, 'login']],
    ['GET', '/logout', [LoginController::class, 'logout'], [
        Authenticate::class
    ]],
    ['GET', '/dashboard', [DashboardController::class, 'index'], [
        Authenticate::class,
        Dummy::class
    ]]
];
