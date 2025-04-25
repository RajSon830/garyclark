<?php 

namespace Raj\Framework\Routing;

use Raj\Framework\Http\Request;

interface RouterInterface{

    public function dispatch(Request $request);

}