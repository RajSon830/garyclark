<?php 

namespace App\Controller;

use App\Controller\Widget;
use Raj\Framework\Http\Response;

class HomeController{

    public function __construct(private Widget $widget)
    {
    }

    public function index(): Response
    {
        $content = "<h1>Hello {$this->widget->name}</h1>";

        return new Response($content);
    }

}