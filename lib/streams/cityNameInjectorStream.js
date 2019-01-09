var through = require('through2');

function create(){
  return through.obj(function(record, enc, next) {
    console.log("Running cityNameInjectorStream");
    
    console.log(JSON.stringify(record, null, 2));
 
      var cityRecordPlaceholder = null;

      var aggregatedFilename = record.source_id.substring(0, record.source_id.indexOf(':'));
      var tokens = aggregatedFilename.split('/');

      if( tokens.length > 0 ) {
        var country = tokens[0];

        if (tokens.length == 3) {
          var region = tokens[1];
          var fileName = tokens[2];

          cityRecordPlaceholder =  region + "," + country;

          if (fileName.startsWith('city_of_')) {
             var cityInFileName = fileName.substr('city_of_'.length).replace('_', ' ');
             record.setMeta('cityInFileName', cityInFileName + "," + region + "," + country);
          }
        } else {
          cityRecordPlaceholder = country;
        }    
    }

    if (record.hasMeta('OACityRecord')) {
      var city = record.getMeta('OACityRecord');
      cityRecordPlaceholder = city + "," + cityRecordPlaceholder;	
      console.log('OACityRecord ' + city);

      record.setMeta('cityInRecord', cityRecordPlaceholder);
    }

    next(null, record);
  });
};


module.exports.create = create;
