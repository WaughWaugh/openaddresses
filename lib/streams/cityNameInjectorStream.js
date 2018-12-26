var through = require('through2');

function create(){
  return through.obj(function(record, enc, next) {
    console.log("Running reocrdInjectorStream");
    
    console.log(JSON.stringify(record, null, 2));
 
    if (record.hasMeta('cityInRecord')) {
      var city = record.getMeta('cityInRecord');
      console.log('CityInRecord ' + city);

      var aggregatedFilename = record.source_id.substring(0, record.source_id.indexOf(':'));
      var tokens = aggregatedFilename.split('/');
 
      if (tokens.length == 3) {
	var country = tokens[0];
        var region = tokens[1];
        var fileName = tokens[2];

	var fullyQualifiedCityName = city + "," + region + "," + country;

	record.setMeta('cityInRecord', fullyQualifiedCityName);

        console.log('CityInRecord ' + record.getMeta('cityInRecord'));

        var cityInFileName = fileName.substr('city_of_'.length).replace('_', ' ');
        record.setMeta('cityInFileName', cityInFileName + "," + region + "," + country);

        console.log("CityInFileName " + record.getMeta('cityInFileName'));
      }
    }

    next(null, record);
  });
};


module.exports.create = create;
