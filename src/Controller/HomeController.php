<?php 

namespace App\Controller;

use App\Controller\Widget;
use Raj\Framework\Http\Response;
use App\Controller\AbstractCotroller;

class HomeController extends AbstractCotroller {

    public function __construct(private Widget $widget)
    {
    }

    public function index(): Response
    {
       
        return $this->render('home.html.twig',['name'=>$this->widget->name]);
    }

}