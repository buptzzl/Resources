var lb=function(options){
var _this=this;
var currentIndex=1;
_this.getObjsByClass=function(tagName,param){
       var tags = document.getElementsByTagName(""+tagName);
       var list = new Array();
       for(var k in tags)
             {
               var tag = tags[k];
               	if(tag.className){
               	var i=0;
                if(tag.className.indexOf(param)>=0) {
                	for(var index in list)
                	{
                		if(list[index]==tag)
                		i+=1;
                	}
                	if(i==0)
                	{
                			list.push(tag);
                	}
                  }
              }}
              return list;
       }

var triggers=this.getObjsByClass("li",options.t);
var stages=this.getObjsByClass("p",options.s);

_this.set1=function(index)
{
	  var el=triggers[index];
	 el.onmouseover=function(){
	 
        currentIndex=parseInt(index)+1;
		clearInterval(_this.interval);
        _this.elOnFocus(index);
    }
    el.onmouseout=function(){
		_this.elLostFocus();
    }
}

for(var index in triggers)
{
    _this.set1(index); 
}


_this.elOnFocus=function(n){
        for(var index in triggers){
 	 	 trigger=triggers[index];
         stage=stages[index];
         if(index!=n){
			trigger.className="yslider-trigger";
 			stage.style.zIndex=1;
         }else{	
	 	 	 trigger.className="yslider-trigger  selected";
        	 stage.style.zIndex=9;
		}
	}
}

_this.elLostFocus=function(el){
      _this.start();
}
_this.start=function(){
_this.interval=setInterval(function(){
		_this.elOnFocus(currentIndex++%triggers.length)
    },options.time);
}
}

