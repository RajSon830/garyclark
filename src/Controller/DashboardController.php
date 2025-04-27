<?php 

namespace App\Controller;

use Raj\Framework\Http\Response;

class DashboardController extends AbstractCotroller{

    public function index():Response{

        return $this->render('dashboard.html.twig');
    }


}