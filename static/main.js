$(function() {
  console.log('jquery is working!Jacot');
  appendWordCloud();
});

function appendWordCloud(){
  var img = document.createElement("img");
  img.src = "/static/wordCloud.png";

  var wordCloud = document.getElementById("wordCloud");
  wordCloud.appendChild(img);
}
