<?php 

namespace App\Controller;

use Raj\Framework\Http\Response;

class PostController extends AbstractCotroller{

    public function show(int $id):Response{

        return $this->render('posts.html.twig',[
            'postId'=>$id
        ]);
    }

    public function create(){

        return $this->render('create-post.html.twig');
    }


}