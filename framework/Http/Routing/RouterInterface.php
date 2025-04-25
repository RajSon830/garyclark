<?php 

namespace Raj\Framework\Http\Routing;

use Raj\Framework\Http\Request;

interface RouterInterface{

    public function dispatch(Request $request);

}